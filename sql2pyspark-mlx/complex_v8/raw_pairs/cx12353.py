from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_month_movi = Window.orderBy(F.col("start_date_month").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_start_date_month_ties = Window.orderBy(F.col("start_date_month").desc())

prepared_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status").isin("ACTIVE", "LAPSED"), "group_a").when(F.col("status") == "CANCELLED", "group_b").otherwise("other").alias("status_group"),
        F.month(F.col("start_date")).alias("start_date_month"),
    )
)

sequenced = (
    prepared_policys
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "status_group",
        F.round(F.avg("premium").over(w_all_by_start_date_month_movi), 2).alias("premium_avg_3"),
        F.rank().over(w_all_by_start_date_month_ties).alias("start_date_month_rank"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["start_date_month_rank", "status", "status_group"], ["start_date_month_rank"], ["status"], []],
        "start_date_month_rank", "status", "status_group",
    )
    .agg(F.sum("start_date_month_rank").alias("total_start_date_month_rank"))
)
