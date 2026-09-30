from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("city").orderBy(F.col("rating_share").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price").between(380, 640)), "left_semi")
)

eligible_suppliers = (
    eligible
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") < "2024-07-01"), "left_anti")
)

enriched = (
    eligible_suppliers
    .select(
        "*",
        F.sum("rating").over(w).alias("running_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_last = (
    deduped
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.category").alias("product_category"), F.col("c.brand"))
)

daily = (
    with_last
    .cube("city")
    .agg(
        F.min("running_rating").alias("min_running_rating"),
        F.sum("running_rating").alias("total_running_rating"),
    )
)

result = (
    daily
    .select("min_running_rating")
    .orderBy("min_running_rating")
)
