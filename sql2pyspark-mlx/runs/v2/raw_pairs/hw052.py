from pyspark.sql import functions as F

result = df_suppliers.select(
    "id",
    F.when(F.col("rating") >= 4.5, "A")
    .when(F.col("rating") >= 3.5, "B")
    .when(F.col("rating") >= 2.5, "C")
    .otherwise("D")
    .alias("grade"),
)
