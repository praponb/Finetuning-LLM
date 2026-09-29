from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "JP"]))
    .select(
        "*",
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.city",
        "s.rating",
        "s.resolved_city",
        F.col("c.category"),
        F.col("c.cost"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand").alias("product_brand"))
)

with_last_suppliers = (
    with_last
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

eligible = (
    with_last_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date") <= F.col("s.launch_date")), "left_anti")
)

summary = (
    eligible
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.max(F.when(F.col("product_brand") == "Globex", F.col("cost"))).alias("globex_max_cost"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

result = summary.select("activity_month", "globex_max_cost", "country_list")
