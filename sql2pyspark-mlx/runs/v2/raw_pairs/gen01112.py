from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("supplier_name").like("%EuroGo%") & (F.col("city") == "London"))
    .groupBy("city")
    .agg(
        F.max("rating").alias("max_rating"),
        F.count("rating").alias("rating_count"),
    )
    .filter(F.col("rating_count") >= 2)
    .drop("rating_count")
)
