from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_singapore", "id")
w2 = Window.partitionBy("city").orderBy("is_singapore", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("is_singapore", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_suppliers
    .filter(F.col("country") == "US")
    .select(
        "id",
        "supplier_name",
        "city",
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.upper(F.col("ch.product_name")).like("%UNN%")), "left_semi")
)

scored = (
    eligible
    .withColumn("next_is_singapore", F.lead("is_singapore", 1).over(w))
    .withColumn("is_singapore_avg_5", F.avg("is_singapore").over(w2))
    .withColumn("last_known_is_singapore", F.last("is_singapore", ignorenulls=True).over(w3))
)

kept = (
    scored
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand").isin(["Globex", "Acme"])), "left_semi")
)

result = (
    kept
    .select("id", "supplier_name", "is_singapore", "city_clean", "is_singapore_avg_5")
    .orderBy(F.col("id").desc())
)
