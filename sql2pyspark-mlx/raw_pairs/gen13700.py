from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("greatest_revenue_units", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date_period", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w4 = Window.partitionBy(F.month("sale_date_period"), "store_id").orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("store_id") == 5)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.date_trunc("week", "sale_date").alias("sale_date_period"),
    )
)

filtered = (
    valid
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-07-21")))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        "sale_date_period",
        F.greatest("revenue", "units").alias("greatest_revenue_units"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_dims = (
    ranked
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .filter(F.col("b.sqft") == 12995)
    .select(
        "e.id",
        "e.store_id",
        "e.units",
        "e.revenue",
        "e.sale_date_period",
        "e.greatest_revenue_units",
        F.col("b.sqft"),
        F.col("b.store_name"),
        F.col("b.opened_date"),
    )
)

enriched = (
    with_dims
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.units",
        "a.revenue",
        "a.sale_date_period",
        "a.store_name",
        F.col("b.sqft").alias("store_sqft"),
    )
)

ranked_store_sales = (
    enriched
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked_store_sales
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        "sale_date_period",
        "store_name",
        "row_num",
        F.ntile(3).over(w3).alias("units_bucket"),
        F.last("store_sqft", ignorenulls=True).over(w4).alias("last_known_store_sqft"),
    )
)

result = (
    metrics
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        "sale_date_period",
        "store_name",
        "row_num",
        "last_known_store_sqft",
        F.when(F.col("store_name").rlike("^(Airport Kiosk|Old Town)$"), "EXTENDED").when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "GROUP_B").otherwise("PRIORITY").alias("store_name_zone"),
    )
    .orderBy(F.col("store_id").desc_nulls_first())
)
