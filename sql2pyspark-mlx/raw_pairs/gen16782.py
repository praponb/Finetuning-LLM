from pyspark.sql import functions as F

rollup_totals = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .rollup(F.col("s.city").alias("city"), F.col("a.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.cost")).alias("min_cost"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    rollup_totals
    .select(
        "city",
        "brand",
        "rows_total",
        F.when(F.col("city").rlike("^(Tokyo|London)$"), "EXTENDED").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy("brand")
)
