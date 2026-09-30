from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("store_id").orderBy(F.col("units_avg_6_prior").desc_nulls_first(), "id")
w4 = Window.partitionBy("store_id").orderBy("store_sqft", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("store_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-20").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.date_add("sale_date", 32).alias("sale_date_plus_32d"),
        F.when(F.col("units") >= 32, "High").when(F.col("units") >= 12, "Medium").otherwise("Low").alias("units_tier"),
    )
)

enriched = (
    filtered
    .filter(F.col("units") < 22)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "sale_date_plus_32d",
        "units_tier",
        F.sum("units").over(w).alias("cumulative_units"),
        F.avg("units").over(w2).alias("units_avg_6_prior"),
    )
)

enriched_store_sales = (
    enriched
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.store_id",
        "e.sale_date",
        "e.units",
        "e.units_tier",
        "e.units_avg_6_prior",
        F.col("d.region"),
        F.col("d.sqft"),
        F.col("d.opened_date"),
    )
)

deduped = (
    enriched_store_sales
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "units_avg_6_prior", "sqft")
)

enriched_store_sales_2 = (
    deduped
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        F.col("p.sqft").alias("store_sqft"),
        F.col("p.store_name"),
    )
)

scored = (
    enriched_store_sales_2
    .withColumn("last3_max_store_sqft", F.max("store_sqft").over(w4))
    .withColumn("last3_count_store_sqft", F.count("store_sqft").over(w4))
    .withColumn("last3_avg_store_sqft", F.avg("store_sqft").over(w4))
    .withColumn("running_store_sqft", F.sum("store_sqft").over(w5))
)

result = (
    scored
    .alias("a")
    .join(df_stores.alias("dim"), F.col("a.store_id") == F.col("dim.id"), "left")
    .groupBy(F.col("dim.region").alias("region"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("a.running_store_sqft")), 2).alias("avg_running_store_sqft"),
    )
)
