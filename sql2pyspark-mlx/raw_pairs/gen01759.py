from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("country").isin(["DE", "SG"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(F.countDistinct("rating").alias("unique_countrys"))
    .filter(F.col("unique_countrys") >= 7)
    .select("country_group", "unique_countrys")
)
