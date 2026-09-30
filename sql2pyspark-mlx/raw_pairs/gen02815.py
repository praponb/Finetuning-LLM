from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-07-02").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "starts_on",
        F.round((F.datediff("starts_on", "ends_on") / 30), 1).alias("months_ends_on_to_starts_on"),
    )
)

result = (
    cleaned
    .select("id")
    .subtract(
        df_promotions
        .filter(F.month("starts_on").isin([2, 5, 7]))
        .select("id")
    )
)
