from pyspark.sql import functions as F

base = (
    df_products
    .filter((~F.col("brand").eqNullSafe(F.lit("Initech"))) & (F.col("price") < 1190))
    .select("*", (F.col("stock_qty") * F.col("price")).alias("stock_qty_times_price"))
)

result = (
    base
    .select(
        "id",
        "category",
        "cost",
        "stock_qty_times_price",
        F.col("price").alias("PRICE_VAL"),
    )
    .unpivot(
        ids=["id", "category"],
        values=["cost", "stock_qty_times_price", "PRICE_VAL"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
)
