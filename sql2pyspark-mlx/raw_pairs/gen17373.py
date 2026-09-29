from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "SG"]))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)

enriched = (
    base
    .withColumn("rating_avg_2", F.avg("rating").over(w))
    .withColumn("rating_bucket", F.ntile(10).over(w2))
)

with_last = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.rating_avg_2", F.col("c.category").alias("product_category"))
)

with_last_suppliers = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"), F.col("c.cost").alias("product_cost"))
)

result = (
    with_last_suppliers
    .groupBy("product_category")
    .agg(
        F.size(F.collect_set("product_category")).alias("num_product_categories"),
        F.sort_array(F.collect_set("product_category")).alias("product_category_list"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc())
)
