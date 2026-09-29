from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("price").alias("min_product_price"))

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.year("launch_date") >= 2025))
    .withColumn("brand_group", F.when(F.col("brand").isin(["Initech", "Acme"]), "Core").otherwise("Other"))
    .withColumn("launch_date_quarter", F.quarter("launch_date"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("OTHER")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

eligible = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity") >= 175), "left_semi")
)

joined = (
    eligible
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.col("p.supplier_name").like("%Gmb%"))
    .select(
        "s.id",
        "s.product_name",
        "s.brand",
        "s.price",
        "s.cost",
        "s.tags",
        "s.supplier_id",
        "s.launch_date_quarter",
        "s.brand_clean",
        "s.inventory_quantity",
        F.col("p.city"),
    )
)

result = (
    joined
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "price",
        "cost",
        "supplier_id",
        "inventory_quantity",
        "city",
        F.when(F.col("city").rlike("^(Singapore|Berlin)$"), "PRIORITY").otherwise("GROUP_B").alias("city_zone"),
        F.col("min_product_price"),
    )
    .orderBy(F.col("supplier_id").desc())
)
