from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("price").desc())
w2 = Window.partitionBy("city").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("supplier_id")
w4 = Window.partitionBy("category").orderBy("rating", "id")
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("stock_qty").alias("sum_product_stock_qty"))

cleaned = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-09-17"))) & (F.col("cost") > 470))
    .select(
        "*",
        F.initcap(F.trim("product_name")).alias("product_name_title"),
        (F.datediff(F.current_date(), "launch_date") / 365.25).alias("years_since_launch_date"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

joined = (
    ranked
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.city"), F.col("d.supplier_name"), F.col("d.rating"))
)

scored = (
    joined
    .withColumn("cost_bucket", F.ntile(3).over(w2))
    .withColumn("supplier_id_stddev_samp_years_since_launch_date", F.stddev_samp("years_since_launch_date").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

result = (
    scored
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "supplier_id",
        "product_name_title",
        "row_num",
        F.when(F.col("product_name").rlike("^(Coffee Maker|Yoga Mat)$"), "GROUP_A").otherwise("GROUP_B").alias("product_name_zone"),
        F.col("sum_product_stock_qty"),
    )
    .orderBy(F.col("row_num").desc())
)
