from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id", "city").orderBy(F.col("rating").desc_nulls_first(), "id")

joined = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.rating"), F.col("p.city"))
)

with_last = (
    joined
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"))
)

ranked = (
    with_last
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

wide = (
    ranked
    .groupBy("brand")
    .pivot("category", ["Fashion", "Grocery", "Home"])
    .agg(F.max("cost").alias("total"), F.count(F.lit(1)).alias("n"))
)

result = (
    wide
    .select("brand", "Fashion_n", "Grocery_total", "Grocery_n", "Home_total")
    .orderBy(F.col("Grocery_total").desc_nulls_first(), F.col("Grocery_n").desc())
)
