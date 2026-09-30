from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("product_cost", "id")
w3 = Window.partitionBy("city").orderBy("product_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("product_cost", "id").rowsBetween(-3, -1)
w5 = Window.orderBy("product_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("product_cost", "id").rowsBetween(-3, -1)
w7 = Window.orderBy("product_cost", "id")
w8 = Window.orderBy("product_price_avg_3_prior", "id").rowsBetween(-2, -1)
w9 = Window.orderBy("next_product_cost", "id")
w10 = Window.orderBy("next_product_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy(F.col("cumulative_product_price_avg_2_prior_minus_next_product_cost_2").desc_nulls_last())
w12 = Window.orderBy(F.col("cumulative_product_price_avg_2_prior_minus_next_product_cost_2").desc(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.trim("country").alias("clean_country"),
        F.lower("supplier_name").alias("supplier_name_lower"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.clean_country",
        "s.supplier_name_lower",
        F.col("c.cost").alias("product_cost"),
        F.col("c.price").alias("product_price"),
    )
)

enriched = (
    latest_match
    .select(
        "id",
        "supplier_name_lower",
        "product_cost",
        "product_price",
        F.lead("product_cost", 1).over(w2).alias("next_product_cost"),
        F.sum("product_price").over(w3).alias("cumulative_product_price"),
        F.avg("product_price").over(w4).alias("product_price_avg_3_prior"),
    )
)

with_windows = (
    enriched
    .withColumn("last_known_product_price_avg_3_prior", F.last("product_price_avg_3_prior", ignorenulls=True).over(w5))
    .withColumn("product_price_avg_3_prior_2", F.avg("product_price").over(w6))
    .withColumn("prev_product_cost", F.lag("product_cost", 2).over(w7))
    .withColumn("last_known_product_price_avg_3_prior_2", F.last("product_price_avg_3_prior", ignorenulls=True).over(w5))
    .withColumn("cumulative_product_price_avg_2_prior", F.avg("cumulative_product_price").over(w8))
)

with_windows_suppliers = (
    with_windows
    .select(
        "id",
        "supplier_name_lower",
        "product_cost",
        "product_price",
        "next_product_cost",
        "cumulative_product_price",
        "last_known_product_price_avg_3_prior",
        "prev_product_cost",
        "last_known_product_price_avg_3_prior_2",
        "cumulative_product_price_avg_2_prior",
        F.lag("last_known_product_price_avg_3_prior").over(w9).alias("prev_last_known_product_price_avg_3_prior"),
        F.lead("last_known_product_price_avg_3_prior").over(w9).alias("next_last_known_product_price_avg_3_prior"),
        F.lag("product_cost").over(w9).alias("prev_product_cost_2"),
        F.lead("product_cost").over(w9).alias("next_product_cost_2"),
        F.row_number().over(w9).alias("event_seq"),
        F.last("product_price_avg_3_prior_2", ignorenulls=True).over(w10).alias("last_known_product_price_avg_3_prior_2_2"),
    )
)

cleaned = (
    with_windows_suppliers
    .filter(F.col("product_cost") > 320)
    .select(
        "id",
        "supplier_name_lower",
        "product_cost",
        "next_product_cost",
        "last_known_product_price_avg_3_prior",
        "last_known_product_price_avg_3_prior_2",
        "prev_last_known_product_price_avg_3_prior",
        "next_last_known_product_price_avg_3_prior",
        "prev_product_cost_2",
        "next_product_cost_2",
        "event_seq",
        "last_known_product_price_avg_3_prior_2_2",
        (F.col("cumulative_product_price_avg_2_prior") - F.col("next_product_cost_2")).alias("cumulative_product_price_avg_2_prior_minus_next_product_cost_2"),
        F.round((F.col("last_known_product_price_avg_3_prior_2") / F.col("cumulative_product_price_avg_2_prior")), 2).alias("last_known_product_price_avg_3_prior_2_per_cumulative_product_price_avg_2_prior"),
    )
)

metrics = (
    cleaned
    .filter(F.col("event_seq") < 8)
    .select(
        "id",
        "supplier_name_lower",
        "next_product_cost",
        "last_known_product_price_avg_3_prior",
        "last_known_product_price_avg_3_prior_2",
        "next_last_known_product_price_avg_3_prior",
        "next_product_cost_2",
        "last_known_product_price_avg_3_prior_2_2",
        F.rank().over(w11).alias("cumulative_product_price_avg_2_prior_minus_next_product_cost_2_rank"),
        F.row_number().over(w12).alias("event_seq_2"),
    )
)

result = (
    metrics
    .cube(
        F.when(F.col("next_product_cost_2") >= 750, "LARGE").otherwise("SMALL").alias("next_product_cost_2_tier"),
    )
    .agg(
        F.percentile_approx("last_known_product_price_avg_3_prior", 0.5).alias("last_known_product_price_avg_3_prior_p50"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
