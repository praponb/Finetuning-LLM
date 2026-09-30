from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "UK") & ~F.col("city").isin(["New York", "London", "Chiang Mai"]))
    .groupBy(
        "country",
        F.when(F.col("country").isin(["UK", "US"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(F.max("rating").alias("max_rating"))
    .filter(F.col("max_rating") < 3.5)
)
