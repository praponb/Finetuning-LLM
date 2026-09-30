from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter((F.col("sqft") >= 4544) & F.col("store_name").like("Ol%") & ~F.col("city").isin(["Tokyo", "Bangkok"]))
)

summary = (
    valid_stores
    .groupBy("city")
    .agg(F.min("opened_date").alias("min_opened_date"))
)

result = (
    summary
    .select("city", "min_opened_date")
    .orderBy(F.col("city").desc())
)
