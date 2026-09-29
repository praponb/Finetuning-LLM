from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-10-02"))) & F.col("opened_date").between("2025-07-05", "2025-08-01"))
    .select(
        "id",
        "store_name",
        "opened_date",
        F.col("sqft").cast("double").alias("sqft_str"),
        F.trim("city").alias("clean_city"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue"),
        F.col("c.sale_date").alias("store_sale_sale_date"),
    )
)

filtered = (
    with_last
    .filter(F.year("opened_date") == 2023)
    .select(
        "*",
        F.datediff("opened_date", "store_sale_sale_date").alias("days_store_sale_sale_date_to_opened_date"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("store_name").isNotNull())
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.store_sale_sale_date")), "left_semi")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") >= F.col("s.opened_date")), "left_anti")
)

not_flagged_stores = (
    kept
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.status") != "PENDING"), "left_semi")
)

result = (
    not_flagged_stores
    .select(
        "id",
        "opened_date",
        "sqft_str",
        "clean_city",
        "store_sale_sale_date",
        "days_store_sale_sale_date_to_opened_date",
    )
    .orderBy(F.col("id").asc_nulls_first(), "days_store_sale_sale_date_to_opened_date")
)
