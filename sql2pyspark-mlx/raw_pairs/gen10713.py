from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-07-17"))) & F.col("opened_date").between("2024-07-06", "2025-09-11"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        F.lower("city").alias("city_lower"),
        F.when(F.col("city").isin(["London", "Tokyo"]), "Tier 1").otherwise("Other").alias("city_group"),
    )
)

scored = (
    valid
    .filter(F.col("region").isin(["North", "South"]))
    .select(
        "*",
        F.first("region", ignorenulls=True).over(w).alias("first_known_region"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.opened_date",
        "s.city_lower",
        "s.city_group",
        "s.first_known_region",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

matched_keys = (
    with_last
    .select("id", "region")
    .intersectAll(
        df_stores
        .filter(F.col("store_name").like("Riv%"))
        .select("id", "region")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.region") == F.col("k.region")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "store_sale_revenue",
        "store_sale_sale_date",
    )
)
