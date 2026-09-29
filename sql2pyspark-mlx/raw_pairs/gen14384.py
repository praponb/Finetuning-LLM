from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_city").orderBy("rating", "id")
w3 = Window.partitionBy("supplier_id").orderBy("running_avg_rating", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

with_dims = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.id",
        "e.product_name",
        "e.brand",
        "e.price",
        "e.launch_date",
        "e.tags",
        "e.supplier_id",
        F.col("b.supplier_name"),
        F.col("b.rating"),
        F.col("b.city"),
    )
)

with_dims_products = (
    with_dims
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.country").isin(["JP", "UK", "TH"]))
    .select(
        "e.id",
        "e.brand",
        "e.price",
        "e.supplier_id",
        "e.supplier_name",
        "e.rating",
        "e.city",
        F.col("b.city").alias("supplier_city"),
        F.col("b.country"),
        F.col("b.supplier_name").alias("supplier_supplier_name"),
    )
)

with_windows = (
    with_dims_products
    .select(
        "*",
        F.max("rating").over(w).alias("running_max_rating"),
        F.avg("rating").over(w).alias("running_avg_rating"),
        F.count("rating").over(w).alias("running_count_rating"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "brand",
        "price",
        "supplier_name",
        "rating",
        "city",
        "supplier_city",
        "supplier_supplier_name",
        "running_max_rating",
        "running_avg_rating",
        F.row_number().over(w2).alias("event_seq"),
        F.lead("price", 2).over(w3).alias("next_price"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

result = (
    with_last
    .select(
        "id",
        "supplier_name",
        "rating",
        "city",
        "supplier_city",
        "supplier_supplier_name",
        "running_max_rating",
        "running_avg_rating",
        "event_seq",
    )
    .orderBy(F.col("id").asc_nulls_first())
    .limit(10)
)
