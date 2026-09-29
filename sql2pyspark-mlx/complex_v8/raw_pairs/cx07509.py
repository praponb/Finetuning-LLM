from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("holder_id", "status").orderBy(F.col("start_date").desc(), F.col("id").desc())

base = (
    df_policies
    .filter(F.col("end_date") < F.lit("2026-10-23").cast("date"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

latest_per_key = (
    base
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .rollup("status", "holder_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("premium").alias("max_premium"),
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("holder_id").asc_nulls_last())
)
