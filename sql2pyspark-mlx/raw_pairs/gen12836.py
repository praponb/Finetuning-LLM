from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("sale_date"), "product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("YEAR", "sale_date_plus_months"), "store_id").orderBy("units", "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w7 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc_nulls_first(), "id")

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-09-17"))) & F.col("product_id").isin([5, 8, 9]))
    .withColumn("revenue_per_unit", (F.col("units") - F.col("revenue")))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 1))
)

metrics = (
    cleaned
    .withColumn("running_max_units", F.max("units").over(w))
    .withColumn("running_count_units", F.count("units").over(w))
    .withColumn("running_sum_units", F.sum("units").over(w))
)

enriched = (
    metrics
    .select(
        "*",
        F.last("running_sum_units", ignorenulls=True).over(w2).alias("last_known_running_sum_units"),
        F.last("running_max_units", ignorenulls=True).over(w3).alias("last_known_running_max_units"),
        F.first("revenue", ignorenulls=True).over(w4).alias("first_known_revenue"),
        F.lead("revenue_per_unit", 1).over(w5).alias("next_revenue_per_unit"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

joined = (
    deduped
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.sale_date",
        "a.units",
        "a.revenue",
        "a.sale_date_plus_months",
        "a.running_count_units",
        "a.last_known_running_sum_units",
        "a.first_known_revenue",
        "a.row_seq",
        "a.rn",
        F.col("p.sqft"),
    )
)

result = (
    joined
    .groupBy("store_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("units").alias("max_units"),
    )
)
