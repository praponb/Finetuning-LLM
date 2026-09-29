from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .groupBy(
        F.when(F.col("city").isin(["Tokyo", "New York"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(F.sum("rating").alias("total_rating"))
)
