from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")
w2 = Window.orderBy("opened_date_plus_months", "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-04-18"))) & F.col("region").isin(["West", "North"]))
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.col("sqft").cast("string").alias("sqft_str"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.opened_date")), "left_anti")
)

filtered_stores = (
    kept
    .filter(F.col("opened_date") > F.lit("2023-02-28").cast("date"))
    .select(
        "id",
        "region",
        "sqft",
        "sqft_str",
        F.add_months("opened_date", 12).alias("opened_date_plus_months"),
    )
)

metrics = (
    filtered_stores
    .filter(F.col("sqft").between(4701, 16031))
    .withColumn("prev_sqft", F.lag("sqft").over(w))
    .withColumn("next_sqft", F.lead("sqft").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

latest_match = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

result = (
    latest_match
    .select("id", "region", "sqft", "sqft_str", "next_sqft", "seq_no")
    .orderBy(F.col("seq_no").asc_nulls_first(), "id")
)
