from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.orderBy("cost", "id")
w4 = Window.partitionBy("brand").orderBy("category_stddev_samp_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("launch_date"), "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("price").desc(), "id")
w7 = Window.partitionBy(F.date_trunc("MONTH", "launch_date"), "supplier_city").orderBy("seq_no", "id")
w8 = Window.partitionBy("city").orderBy("category_stddev_samp_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_products
    .filter(F.col("price").between(370, 620))
    .select(
        "*",
        F.stddev_samp("price").over(w).alias("category_stddev_samp_price"),
        F.percent_rank().over(w2).alias("cost_percent_rank"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

with_windows = (
    scored
    .withColumn("running_seq_no", F.sum("seq_no").over(w4))
    .withColumn("running_cost", F.sum("cost").over(w5))
    .withColumn("seq_no_2", F.row_number().over(w6))
)

base = (
    with_windows
    .filter(F.col("stock_qty") > 179)
    .select(
        "*",
        (F.col("category_stddev_samp_price") + F.col("seq_no")).alias("category_stddev_samp_price_plus_seq_no"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.rating"), F.col("p.supplier_name"), F.col("p.city"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.launch_date",
        "s.supplier_id",
        "s.category_stddev_samp_price",
        "s.cost_percent_rank",
        "s.seq_no",
        "s.running_seq_no",
        "s.running_cost",
        "s.seq_no_2",
        "s.rating",
        "s.city",
        F.col("b.city").alias("supplier_city"),
        F.col("b.supplier_name").alias("supplier_supplier_name"),
        F.col("b.country"),
    )
)

kept = (
    with_dims
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([1, 9, 10])), "left_anti")
)

kept_products = (
    kept
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left_anti")
)

result = (
    kept_products
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "stock_qty",
        "supplier_id",
        "category_stddev_samp_price",
        "cost_percent_rank",
        "seq_no",
        "supplier_supplier_name",
        "country",
        F.lag("price", 1).over(w7).alias("prev_price"),
        F.sum("cost_percent_rank").over(w8).alias("running_cost_percent_rank"),
    )
)
