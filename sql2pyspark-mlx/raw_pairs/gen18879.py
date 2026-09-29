from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w_grp = Window.partitionBy("product_id").orderBy("units", "id")
w3 = Window.partitionBy("product_id").orderBy("event_seq_filled", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("next_revenue_whole").desc(), "id")
w5 = Window.partitionBy(F.year("sale_date")).orderBy(F.col("next_revenue_whole").desc(), "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("event_seq_filled").asc_nulls_last())
w7 = Window.partitionBy(F.year("sale_date"), "product_id")
w8 = Window.partitionBy("product_id").orderBy(F.col("event_seq").desc_nulls_last(), "id")

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-05-06").cast("date")) & F.col("units").between(26, 30))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.floor("revenue").alias("revenue_whole"),
    )
)

scored = (
    filtered
    .filter(F.col("sale_date").between("2023-08-01", "2023-10-01"))
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue_whole",
        F.row_number().over(w).alias("event_seq"),
        F.lead("revenue_whole", 1, 0).over(w_grp).alias("next_revenue_whole"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.brand"), F.col("b.product_name"))
)

filtered_store_sales = (
    enriched
    .filter((F.col("sale_date") > F.lit("2023-05-24").cast("date")) & F.col("brand").isNotNull())
    .select(
        "*",
        F.coalesce("event_seq", F.lit(0)).alias("event_seq_filled"),
        F.date_format("sale_date", "yyyy-MM").alias("sale_date_label"),
    )
)

with_dims = (
    filtered_store_sales
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.sale_date",
        "src.event_seq",
        "src.next_revenue_whole",
        "src.event_seq_filled",
        "src.sale_date_label",
        F.col("d.product_name").alias("product_product_name"),
    )
)

metrics = (
    with_dims
    .select(
        "*",
        F.row_number().over(w3).alias("seq_no"),
        F.ntile(3).over(w4).alias("next_revenue_whole_bucket"),
        F.ntile(3).over(w5).alias("next_revenue_whole_bucket_2"),
        F.percent_rank().over(w6).alias("event_seq_filled_percent_rank"),
        F.count("event_seq_filled").over(w7).alias("sale_date_year_count_event_seq_filled"),
    )
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select("id", "product_id", "event_seq_filled", "event_seq_filled_percent_rank")
    .unpivot(
        ids=["id", "product_id"],
        values=["event_seq_filled", "event_seq_filled_percent_rank"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
