from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

latest_match = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("order_item_quantity"))
)

result = (
    latest_match
    .withColumn("quarter_start", F.date_trunc("QUARTER", "launch_date"))
    .withColumn("activity_month", F.date_trunc("MONTH", "launch_date"))
    .groupingSets(
        [["quarter_start", "activity_month"], ["quarter_start"], []],
        "quarter_start",
        "activity_month",
    )
    .agg(
        F.percentile_approx("order_item_quantity", [0.25, 0.5, 0.75]).alias("order_item_quantity_quartiles"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
