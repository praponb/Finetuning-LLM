from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("launch_date", "id")
w2 = Window.partitionBy("quarter_start").orderBy(F.col("south_events").desc())
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.max("units").alias("max_store_sale_units"))

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2024-04-30").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.floor("revenue").alias("revenue_whole"),
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
    )
)

with_dims = (
    valid
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.region"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.launch_date"), F.col("p.cost"), F.col("p.brand"))
)

deduped = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

valid_store_sales = (
    deduped
    .filter(F.col("region").isNotNull())
    .withColumn("units_plus_greatest_units_revenue", (F.col("units") + F.col("greatest_units_revenue")))
    .withColumn("region_filled", F.coalesce("region", F.lit("UNKNOWN")))
)

joined_store_sales = (
    valid_store_sales
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.sqft"))
)

summary = (
    joined_store_sales
    .groupBy(
        F.date_trunc("QUARTER", "sale_date").alias("quarter_start"),
        "store_id",
        "brand",
    )
    .agg(
        F.count_if(F.col("region") == "South").alias("south_events"),
        F.sum(F.when(F.col("region") == "South", F.col("units")).otherwise(0)).alias("south_units"),
        F.sum(F.when(F.col("region") == "East", F.col("greatest_units_revenue")).otherwise(0)).alias("east_greatest_units_revenue"),
        F.round(F.sum("greatest_units_revenue") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("greatest_units_revenue_per_initech"),
        F.count("*").alias("cnt"),
        F.max("greatest_units_revenue").alias("max_greatest_units_revenue"),
    )
    .withColumn("south_events_percent_rank", F.percent_rank().over(w2))
)

result = (
    summary
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "quarter_start",
        "store_id",
        "brand",
        "south_events",
        "south_units",
        "east_greatest_units_revenue",
        "greatest_units_revenue_per_initech",
        "cnt",
        "max_greatest_units_revenue",
        F.when(F.col("brand").rlike("^(Umbrella|Globex)$"), "GROUP_B").when(F.col("brand").rlike("^(Acme|Initech)$"), "CORE").otherwise("EXTENDED").alias("brand_zone"),
        F.col("max_store_sale_units"),
    )
)
