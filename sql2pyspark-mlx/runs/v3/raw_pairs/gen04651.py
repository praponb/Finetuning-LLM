from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("city").isin(["Tokyo", "Berlin"]), "priority").otherwise("other").alias("city_group"),
        "country",
    )
    .agg(F.max("rating").alias("max_rating"))
    .filter(F.col("max_rating") > 4.1)
    .orderBy("max_rating")
)
