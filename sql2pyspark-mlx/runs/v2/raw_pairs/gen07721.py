from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("city").isin(["Tokyo", "Singapore", "London"]) | F.col("country").isin(["TH", "UK", "JP"]))
)

summary = (
    recent_suppliers
    .groupBy("city", "country")
    .agg(F.max("rating").alias("max_rating"))
)

result = summary.select("city", "country")
