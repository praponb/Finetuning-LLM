from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 2.9)
    .select(
        "country",
        F.round((F.col("rating") * 1.07), 2).alias("rating_scaled"),
        F.round((F.col("rating") * 100), 2).alias("rating_scaled_2"),
    )
    .orderBy(F.col("country").desc())
)
