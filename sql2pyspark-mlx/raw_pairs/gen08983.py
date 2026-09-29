from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("city") == "New York")
    .withColumn("store_name_slug", F.regexp_replace("store_name", " ", ""))
    .withColumn("region_filled", F.coalesce("region", F.lit("NONE")))
)

with_last = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.opened_date",
        F.col("c.sale_date").alias("store_sale_sale_date"),
        F.col("c.units").alias("store_sale_units"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") >= F.col("s.opened_date")), "left_anti")
)

with_last_stores = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.store_sale_units",
        F.col("c.status"),
        F.col("c.order_date"),
    )
)

result = (
    with_last_stores
    .select(
        "id",
        "region",
        "store_sale_units",
        "status",
        F.when(F.col("region").rlike("^(South|Central)$"), "EXTENDED").when(F.col("region").rlike("^(North|East)$"), "PRIORITY").otherwise("GROUP_A").alias("region_zone"),
    )
)
