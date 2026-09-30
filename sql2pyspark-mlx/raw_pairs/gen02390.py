from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

scored = (
    df_suppliers
    .filter(F.col("city") != "Chiang Mai")
    .select(
        "id",
        "supplier_name",
        "rating",
        F.avg("rating").over(w).alias("rating_avg_6"),
        F.sum("rating").over(w2).alias("city_sum_rating"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") <= 540), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.cost").alias("product_cost"),
        F.col("c.category").alias("product_category"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.rating_avg_6",
        "s.city_sum_rating",
        "s.product_category",
        F.col("c.brand"),
        F.col("c.launch_date").alias("product_launch_date"),
    )
)

kept_suppliers = (
    with_last
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.launch_date") < F.col("s.product_launch_date")), "left_anti")
)

totals = (
    kept_suppliers
    .groupBy("product_category", "brand")
    .agg(
        F.size(F.collect_set("product_category")).alias("num_product_categories"),
        F.avg("city_sum_rating").alias("avg_city_sum_rating"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("city_sum_rating")).otherwise(0)).alias("acme_city_sum_rating"),
        F.count(F.when(F.col("product_category") == "Grocery", 1)).alias("grocery_rows"),
        F.max("city_sum_rating").alias("max_city_sum_rating"),
        F.max(F.when(F.col("product_category") == "Sports", F.col("rating_avg_6"))).alias("sports_max_rating_avg_6"),
    )
)

result = (
    totals
    .select(
        "product_category",
        "brand",
        "num_product_categories",
        "acme_city_sum_rating",
        "grocery_rows",
        "max_city_sum_rating",
        "sports_max_rating_avg_6",
        F.when(F.col("product_category").rlike("^(Home|Books)$"), "OTHER_ZONE").when(F.col("product_category").rlike("^(Electronics|Sports)$"), "CORE").otherwise("EXTENDED").alias("product_category_zone"),
    )
)
