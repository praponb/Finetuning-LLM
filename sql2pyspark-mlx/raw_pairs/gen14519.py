from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter((~F.col("region").eqNullSafe(F.lit("South"))) & (F.col("opened_date") > F.lit("2023-06-01").cast("date")))
    .select(
        "id",
        "city",
        "region",
        "sqft",
        (F.datediff(F.current_date(), "opened_date") / 365.25).alias("years_since_opened_date"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

result = (
    filtered
    .select("id", "region")
    .exceptAll(
        df_stores
        .filter(F.col("opened_date") <= "2025-08-01")
        .select("id", "region")
    )
)
