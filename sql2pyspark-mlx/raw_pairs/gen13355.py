from pyspark.sql import functions as F

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-05-30"))) & F.col("region").isNotNull())
    .withColumn("city_upper", F.upper("city"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("UNKNOWN")))
)

result = (
    valid
    .select("region", "id")
    .subtract(
        df_stores
        .filter(F.col("region") == "South")
        .select("region", "id")
    )
)
