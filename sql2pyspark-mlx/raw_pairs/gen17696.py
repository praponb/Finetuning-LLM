from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").asc_nulls_first(), "id")
w2 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("WEEK", "launch_date"), "brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter(F.col("supplier_id") == 9)
    .select("*", (F.col("price") + F.col("cost")).alias("margin"))
)

cleaned = (
    valid
    .filter((F.col("launch_date") > F.lit("2023-09-01").cast("date")) & (F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "*",
        F.concat("product_name", F.lit(" "), "brand").alias("product_name_brand"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    deduped
    .filter(F.col("supplier_id").isin([4, 5, 6, 36]))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "margin",
        "product_name_brand",
        "latest_rank",
        F.sum("stock_qty").over(w2).alias("running_stock_qty"),
        F.last("price", ignorenulls=True).over(w3).alias("last_known_price"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("year", "launch_date").alias("year_start"))
    .agg(
        F.count_if(F.col("category") == "Grocery").alias("grocery_events"),
        F.stddev_samp("price").alias("price_stddev"),
    )
    .filter(F.col("price_stddev") > 0)
    .orderBy(F.col("price_stddev").asc_nulls_last())
)
