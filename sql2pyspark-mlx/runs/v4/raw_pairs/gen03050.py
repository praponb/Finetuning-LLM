from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc_nulls_first(), "id")

valid = (
    df_products
    .filter((F.col("launch_date") > F.lit("2024-01-02").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.upper("brand").alias("brand_upper"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.launch_date")), "left_anti")
)

with_dims = (
    kept
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"), F.col("d.supplier_name"))
)

deduped = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_dims_products = (
    deduped
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "left")
    .select(
        "s.*",
        F.col("p2.supplier_name").alias("supplier_supplier_name"),
        F.col("p2.country"),
        F.col("p2.rating"),
    )
)

result = (
    with_dims_products
    .select(
        "id",
        "category",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "brand_upper",
        "resolved_brand",
        "city",
        "supplier_name",
    )
    .orderBy(F.col("launch_date").desc_nulls_last())
)
