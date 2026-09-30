from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_status_not_in_lapsed_expir = Window.partitionBy("n_status_not_in_lapsed_expired", "holder_id")
w_all_by_min_premium_running = Window.orderBy(F.col("min_premium").desc(), "holder_id", "start_date_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_min_premium = Window.orderBy(F.col("min_premium").desc(), "holder_id", "start_date_month")

scoped_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status").isin("ACTIVE", "CANCELLED"), "group_a").when(F.col("status") == "EXPIRED", "group_b").otherwise("other").alias("status_group"),
    )
)

grouped = (
    scoped_policys
    .groupBy(
        "holder_id",
        F.date_trunc("month", "start_date").alias("start_date_month"),
    )
    .agg(
        F.count(F.when(~F.col("status").isin("LAPSED", "EXPIRED"), 1)).alias("n_status_not_in_lapsed_expired"),
        F.min("premium").alias("min_premium"),
    )
)

sequenced = (
    grouped
    .select(
        "*",
        F.first("min_premium", ignorenulls=True).over(w_all_by_min_premium_running).alias("first_known_min_premium"),
        F.sum("min_premium").over(w_all_by_min_premium_running).alias("running_min_premium"),
        F.rank().over(w_all_by_min_premium).alias("min_premium_rank"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.year(F.col("p.joined_on")) == 2024) & (F.col("p.holder_name").isNotNull()), "left_anti")
)

result = (
    qualified
    .select(
        "holder_id",
        "start_date_month",
        "n_status_not_in_lapsed_expired",
        "min_premium_rank",
    )
    .unpivot(
        ids=["holder_id", "start_date_month"],
        values=["n_status_not_in_lapsed_expired", "min_premium_rank"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
