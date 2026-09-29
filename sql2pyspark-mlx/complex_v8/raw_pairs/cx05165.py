from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("status").orderBy(F.col("start_date").desc(), F.col("id").desc())

filtered_policys = (
    df_policies
    .filter((F.col("start_date").between(F.lit("2024-03-27").cast("date"), F.lit("2025-05-02").cast("date"))) & (F.col("holder_id") >= 4))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

tagged = (
    filtered_policys
    .select(
        "id",
        "premium",
        "start_date",
        "status",
        "status_slug",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug_2"),
    )
)

unique_rows = (
    tagged
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

labeled = (
    unique_rows
    .filter(F.col("premium").between(1051.9, 4417.0))
    .select("*", F.quarter(F.col("start_date")).alias("start_date_quarter"))
)

result = (
    labeled
    .select(
        "id",
        "premium",
        "status",
        "status_slug",
        "start_date_quarter",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("start_date")).alias("days_since_start_date"),
    )
    .orderBy(F.col("status").asc(), "status_slug")
)
