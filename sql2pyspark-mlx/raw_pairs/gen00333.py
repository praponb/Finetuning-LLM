from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") <= F.col("s.product_launch_date")), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.city", F.col("c.category"))
)

eligible_suppliers = (
    latest_match
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.stock_qty") >= 220), "left_anti")
)

not_flagged = (
    eligible_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date").between("2023-09-01", "2023-11-13")), "left_anti")
)

latest_match_suppliers = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.category", F.col("c.category").alias("product_category"))
)

kept = (
    latest_match_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("ch.tags"), "eco")), "left_anti")
)

result = (
    kept
    .groupBy("product_category")
    .agg(
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.count_if(F.col("category") == "Sports").alias("sports_events"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .filter(F.col("sports_events") < 1000)
    .orderBy(F.col("sports_events").desc_nulls_last())
)
