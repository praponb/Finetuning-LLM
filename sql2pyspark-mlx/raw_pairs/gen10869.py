from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "stock_qty",
        "tags",
        "supplier_id",
        F.coalesce("product_name", F.lit("N/A")).alias("product_name_filled"),
        F.when(F.col("brand") == "Acme", 1).otherwise(0).alias("is_acme"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "product_name_filled")
)

with_latest = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.revenue"))
)

filtered = (
    with_latest
    .filter(F.col("sale_date") < "2023-10-09")
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
)

with_latest_products = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"), F.col("c.sale_date").alias("store_sale_sale_date"))
)

result = (
    with_latest_products
    .select("id", "revenue", F.col("revenue_filled").alias("REVENUE_FILLED"))
    .unpivot(
        ids=["id"],
        values=["revenue", "REVENUE_FILLED"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
