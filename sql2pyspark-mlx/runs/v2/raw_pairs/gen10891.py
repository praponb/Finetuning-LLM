from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Do%") & (F.col("city") == "Singapore"))
    .select(
        "city",
        F.initcap(F.trim("region")).alias("region_title"),
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
    )
    .orderBy(F.col("region_title").desc())
)
