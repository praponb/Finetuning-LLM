from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category"))
)

result = (
    with_last
    .groupBy(
        F.when(F.col("rating") >= 4.2, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
    )
    .filter(F.col("total_rating") < 0)
)
