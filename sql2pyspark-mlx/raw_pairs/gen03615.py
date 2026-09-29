from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w_hist = Window.partitionBy("country", "city").orderBy("product_cost", "id")
w4 = Window.partitionBy("country").orderBy("product_cost", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("city").orderBy("product_cost_bucket", "id").rowsBetween(-2, Window.currentRow)

latest_match = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        "s.city",
        "s.rating",
        F.col("c.cost").alias("product_cost"),
    )
)

ranked = (
    latest_match
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.month(F.col("ch.launch_date")).isin([1, 3, 5])), "left_anti")
)

with_windows = (
    eligible
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.lead("product_cost").over(w_hist).alias("next_product_cost"),
        F.lag("product_cost").over(w_hist).alias("prev_product_cost"),
        F.lag("rating").over(w_hist).alias("prev_rating"),
        F.lead("rating").over(w_hist).alias("next_rating"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.ntile(10).over(w4).alias("product_cost_bucket"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.city",
        "s.rating",
        "s.prev_product_cost",
        "s.prev_rating",
        "s.next_rating",
        "s.row_seq",
        "s.product_cost_bucket",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.stock_qty"),
    )
)

result = (
    with_last
    .select(
        "id",
        "city",
        "next_rating",
        "row_seq",
        "product_cost_bucket",
        "stock_qty",
        F.max("row_seq").over(w6).alias("last3_max_row_seq"),
        F.avg("row_seq").over(w6).alias("last3_avg_row_seq"),
        F.min("row_seq").over(w6).alias("last3_min_row_seq"),
    )
)
