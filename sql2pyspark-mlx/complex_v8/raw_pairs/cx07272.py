from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status").orderBy(F.col("start_date").desc(), F.col("id").desc())

recent_policys = (
    df_policies
    .filter(F.col("holder_id") <= 17)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.round(F.col("premium") * 0.25, 2).alias("premium_adj"),
    )
)

unique_rows = (
    recent_policys
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .cube("status", "holder_id")
    .agg(
        F.grouping("status").alias("is_total_status"),
        F.max("premium_adj").alias("max_premium_adj"),
    )
    .select(
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "holder_id",
        "is_total_status",
        "max_premium_adj",
    )
)
