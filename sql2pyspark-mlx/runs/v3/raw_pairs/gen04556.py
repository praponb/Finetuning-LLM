from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["TH", "UK"]))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.substring("country", 1, 2).alias("country_prefix"),
        F.trim("city").alias("clean_city"),
    )
    .orderBy("country")
)
