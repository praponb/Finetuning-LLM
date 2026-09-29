from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("stock_qty_minus_price", (F.col("stock_qty") - F.col("price")))
    .withColumn("category_upper", F.upper("category"))
)

with_dims = (
    base
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.country"))
)

filtered = (
    with_dims
    .filter((F.col("launch_date") > F.lit("2023-09-25").cast("date")) & F.array_contains("tags", "new"))
    .select("*", F.greatest("price", "cost").alias("greatest_price_cost"))
)

result = (
    filtered
    .select(
        "id",
        "category",
        F.col("cost").alias("COST_VAL"),
        "greatest_price_cost",
        "price",
    )
    .unpivot(
        ["id", "category"],
        ["COST_VAL", "greatest_price_cost", "price"],
        "kpi",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
