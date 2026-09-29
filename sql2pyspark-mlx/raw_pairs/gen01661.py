from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty"), F.col("c.brand"))
)

result = (
    with_last
    .groupBy("brand")
    .agg(
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("stock_qty_per_initech"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count_if(F.col("brand") == "Acme").alias("acme_events"),
    )
    .orderBy(F.col("rating_stddev").desc_nulls_first())
)
