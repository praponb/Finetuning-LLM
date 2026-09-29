from pyspark.sql import functions as F

cleaned = (
    df_promotions
    .filter(F.col("starts_on") >= F.lit("2023-10-14").cast("date"))
    .select(
        "id",
        "channel",
        "starts_on",
        F.add_months("ends_on", 3).alias("ends_on_plus_months"),
    )
)

result = (
    cleaned
    .select("id", "channel")
    .exceptAll(
        df_promotions
        .filter(F.col("starts_on") <= "2024-11-01")
        .select("id", "channel")
    )
)
