from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        F.when(F.col("rating") >= 4.6, "High").when(F.col("rating") >= 1.6, "Medium").otherwise("Low").alias("rating_level"),
        F.regexp_replace("supplier_name", " ", "-").alias("supplier_name_slug"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand").isin(["Initech", "Globex"])), "left_anti")
)

cleaned = (
    eligible
    .filter(~F.col("city").isin(["Tokyo", "Berlin"]))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating_level",
        "supplier_name_slug",
        F.lower("city").alias("city_lower"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("product_cost"), F.col("c.price"))
)

result = (
    with_last
    .select("id", "supplier_name", "city_lower", "city_clean", "price")
    .orderBy(F.col("id").desc())
)
