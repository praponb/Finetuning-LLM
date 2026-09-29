from pyspark.sql import functions as F

cleaned = (
    df_policies
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
    )
)

result = (
    enriched
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date_quarter",
        F.date_format(F.col("start_date"), "yyyy-MM").alias("start_date_label"),
        F.date_add(F.col("start_date"), 365).alias("start_date_plus_365d"),
    )
    .orderBy(F.col("holder_id").desc(), F.col("start_date_plus_365d").desc(), "id")
    .limit(5)
)
