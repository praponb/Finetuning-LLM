from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "London"]))
    .select(
        "id",
        "supplier_name",
        "country",
        F.lower("city").alias("city_lower"),
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
    .orderBy("city_lower", F.col("supplier_name").desc(), "id")
    .limit(5)
)
