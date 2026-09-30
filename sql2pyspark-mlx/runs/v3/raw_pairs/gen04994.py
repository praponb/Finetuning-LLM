from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(F.when(F.col("country").isin(["SG", "US"]), "group_a").otherwise("other").alias("country_group"))
    .pivot("city", ["Bangkok", "Tokyo"])
    .agg(F.max("rating"))
    .orderBy("country_group")
)
