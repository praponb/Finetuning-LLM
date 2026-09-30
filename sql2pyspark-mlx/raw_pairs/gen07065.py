from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "country").orderBy("city_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "city")

with_dims = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.country"), F.col("p.city"))
)

with_last = (
    with_dims
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"))
)

cleaned = (
    with_last
    .filter(~F.col("brand").isin(["Umbrella", "Initech"]))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "tags",
        "country",
        "city",
        F.length("city").alias("city_length"),
        F.round((F.col("cost") / F.col("revenue")), 2).alias("cost_per_revenue"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "country",
        "city",
        "city_length",
        "city_filled",
        F.first("cost", ignorenulls=True).over(w2).alias("first_known_cost"),
        F.sum("cost").over(w3).alias("country_sum_cost"),
    )
)

remaining_keys = (
    metrics
    .select("brand", "id")
    .exceptAll(
        df_products
        .filter(F.col("price") < 370)
        .select("brand", "id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("brand", "city")
    .pivot("country", ["JP", "US"])
    .agg(F.max("price").alias("total"))
)
