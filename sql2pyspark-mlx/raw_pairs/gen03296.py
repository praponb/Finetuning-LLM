from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") <= 670), "left_anti")
)

ranked = (
    kept
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "supplier_name")
)

base = (
    ranked
    .filter(F.col("supplier_name").like("%Trader%"))
    .withColumn("supplier_name_upper", F.upper("supplier_name"))
    .withColumn("supplier_name_length", F.length("supplier_name"))
)

kept_suppliers = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "bestseller")), "left_anti")
)

result = (
    kept_suppliers
    .groupBy(
        F.when(F.col("supplier_name_length") >= 65, "HIGH").otherwise("LOW").alias("supplier_name_length_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("supplier_name_length", [0.25, 0.5, 0.75]).alias("supplier_name_length_quartiles"),
    )
    .orderBy(F.col("supplier_name_length_tier").asc_nulls_last())
)
