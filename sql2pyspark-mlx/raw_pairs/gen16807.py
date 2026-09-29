from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("city_length").desc())
w2 = Window.partitionBy("region")

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-09-27").cast("date")) & F.col("city").isin(["Tokyo", "Bangkok"]))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.concat("region", F.lit(", "), "store_name").alias("region_store_name"),
    )
)

filtered = (
    valid
    .filter(F.col("opened_date") > F.lit("2023-06-24").cast("date"))
    .withColumn("sqft_band", F.when(F.col("sqft") >= 17824, "High").when(F.col("sqft") >= 5637, "Medium").otherwise("Low"))
    .withColumn("city_length", F.length("city"))
)

kept = (
    filtered
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.opened_date")), "left_anti")
)

remaining_keys = (
    kept
    .select("id")
    .intersect(
        df_stores
        .filter(~F.col("city").isin(["Singapore", "London", "New York"]))
        .select("id")
    )
)

verified = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "id",
        "region",
        "sqft",
        "region_store_name",
        "sqft_band",
        F.cume_dist().over(w).alias("city_length_cume_dist"),
        F.sum("city_length").over(w2).alias("region_sum_city_length"),
    )
)

result = (
    enriched
    .select(
        "id",
        "sqft",
        "region_store_name",
        "city_length_cume_dist",
        "region_sum_city_length",
    )
    .orderBy(F.col("city_length_cume_dist").desc(), "id")
)
