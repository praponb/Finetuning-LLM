from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.orderBy("last_restock", "id")
w_part = Window.orderBy("stock_qty", "id")
w_recent = Window.partitionBy("supplier_id").orderBy("last_restock", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")

not_flagged = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") < F.col("s.launch_date")), "left_anti")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        F.col("c.last_restock"),
    )
)

enriched = (
    with_latest
    .select(
        "id",
        "category",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "last_restock",
        F.row_number().over(w2).alias("seq_no"),
        F.lag("stock_qty", 2, 0).over(w_part).alias("prev_stock_qty"),
        F.avg("stock_qty").over(w_recent).alias("stock_qty_avg_2_prior"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("id", "category", "stock_qty", "tags", "last_restock", "prev_stock_qty")
)

kept_keys = (
    latest
    .select("id", "category")
    .subtract(
        df_products
        .filter(F.col("cost") <= 270)
        .select("id", "category")
    )
)

verified = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "category",
        "stock_qty",
        "tags",
        "prev_stock_qty",
        F.when(F.col("category").rlike("^(Sports|Grocery)$"), "GROUP_B").when(F.col("category").rlike("^(Electronics|Home)$"), "CORE").otherwise("GROUP_A").alias("category_zone"),
    )
    .orderBy(F.col("stock_qty").asc_nulls_first())
)
