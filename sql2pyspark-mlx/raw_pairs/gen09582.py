from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "US")
    .select(
        "city",
        F.initcap(F.trim("country")).alias("country_title"),
        F.round((F.col("rating") * 100), 2).alias("rating_adjusted"),
    )
)
