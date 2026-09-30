from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("South"))))
    .select("*", F.upper("store_name").alias("store_name_upper"))
)

eligible = (
    valid
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([1, 6])), "left_semi")
)

cleaned = (
    eligible
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-04-09")))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "store_name_upper",
        F.when(F.col("sqft") >= 13319, "High").when(F.col("sqft") >= 6314, "Medium").otherwise("Low").alias("sqft_tier"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.opened_date",
        "s.sqft",
        "s.store_name_upper",
        F.col("c.units").alias("store_sale_units"),
        F.col("c.revenue"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        "store_name_upper",
        "store_sale_units",
        "revenue",
    )
)
