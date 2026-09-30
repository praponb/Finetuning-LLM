from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty"), F.col("c.price"))
)

with_windows = (
    with_last
    .select(
        "*",
        F.sum("price").over(w2).alias("cumulative_price"),
        F.first("price", ignorenulls=True).over(w3).alias("first_known_price"),
    )
)

remaining_keys = (
    with_windows
    .select("id", "city")
    .exceptAll(
        df_suppliers
        .filter(F.col("country") == "UK")
        .select("id", "city")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("country", "rating", "price", "first_known_price")
    .unpivot(["country"], ["rating", "price", "first_known_price"], "measure", "val")
    .filter(F.col("val").isNotNull())
)
