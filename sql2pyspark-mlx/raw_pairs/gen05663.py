from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .groupBy(
        F.when(F.col("country").isin(["DE", "TH"]), "group_a").otherwise("other").alias("country_group"),
        "country",
    )
    .agg(F.min("age").alias("min_age"))
)
