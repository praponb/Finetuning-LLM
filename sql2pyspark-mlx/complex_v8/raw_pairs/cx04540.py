from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_start_date_ties = Window.orderBy("start_date")

base = (
    df_policies
    .filter(F.col("holder_id") >= 15)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

filtered = (
    base.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status") != "ACTIVE"), "left_anti")
    .filter(F.col("holder_id").isNotNull())
)

sequenced = (
    filtered
    .select(
        "id",
        "holder_id",
        "start_date",
        "status",
        F.max("premium").over(w_status).alias("status_max_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_status), 2).alias("premium_vs_status_avg"),
        F.max("premium").over(w_all_by_start_date_running).alias("max_premium_so_far"),
        F.lag("premium", 3).over(w_all_by_start_date).alias("prev3_premium"),
        F.round(F.percent_rank().over(w_all_by_start_date_ties), 3).alias("start_date_percent_rank"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["holder_id", "status"], ["holder_id"]],
        "holder_id", "status",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("max_premium_so_far").alias("total_max_premium_so_far"),
    )
)
