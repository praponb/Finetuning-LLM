from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("stock_qty").asc_nulls_first())
w3 = Window.partitionBy("city").orderBy(F.col("stock_qty").asc_nulls_first())
w4 = Window.partitionBy(F.year("inventory_last_restock"), "brand")
w5 = Window.partitionBy("brand").orderBy(F.col("stock_qty").asc_nulls_last())
w6 = Window.partitionBy("brand", "city").orderBy(F.col("stock_qty_percent_rank").desc(), "id")
w7 = Window.partitionBy("city").orderBy("stock_qty", "id")
w8 = Window.partitionBy("city").orderBy("stock_qty_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-04-03").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("category") == "Grocery", 1).otherwise(0).alias("is_grocery"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"))
)

with_last = (
    with_dims
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.stock_qty",
        "s.tags",
        "s.city",
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

metrics = (
    with_last
    .withColumn("stock_qty_dense_rank", F.dense_rank().over(w2))
    .withColumn("stock_qty_percent_rank", F.percent_rank().over(w3))
    .withColumn("inventory_last_restock_year_max_stock_qty", F.max("stock_qty").over(w4))
)

with_windows = (
    metrics
    .filter(F.col("stock_qty").between(202, 386))
    .select(
        "id",
        "category",
        "tags",
        "city",
        "inventory_last_restock",
        "stock_qty_dense_rank",
        "stock_qty_percent_rank",
        "inventory_last_restock_year_max_stock_qty",
        F.rank().over(w5).alias("stock_qty_rank"),
        F.ntile(3).over(w6).alias("stock_qty_percent_rank_bucket"),
        F.ntile(4).over(w7).alias("stock_qty_bucket"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "category",
        "city",
        "inventory_last_restock",
        "stock_qty_dense_rank",
        "stock_qty_percent_rank",
        "inventory_last_restock_year_max_stock_qty",
        "stock_qty_bucket",
        F.min("stock_qty_rank").over(w8).alias("running_min_stock_qty_rank"),
        F.sum("stock_qty_rank").over(w8).alias("running_sum_stock_qty_rank"),
        F.count("stock_qty_rank").over(w8).alias("running_count_stock_qty_rank"),
    )
)

kept_keys = (
    scored
    .select("category", "id")
    .intersectAll(
        df_products
        .filter(F.col("price").between(380, 690))
        .select("category", "id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "city",
        "inventory_last_restock",
        "stock_qty_dense_rank",
        "stock_qty_percent_rank",
        "inventory_last_restock_year_max_stock_qty",
        "running_min_stock_qty_rank",
        F.when(F.col("city").rlike("^(Singapore|Berlin)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("running_min_stock_qty_rank").desc())
)
