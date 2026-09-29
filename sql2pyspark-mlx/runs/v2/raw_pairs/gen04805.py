from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .filter(F.col("product_name").like("Ru%"))
    .select(
        "id",
        "brand",
        "cost",
        "supplier_id",
        "launch_date",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
        F.sum("cost").over(w2).alias("running_cost"),
    )
    .orderBy("launch_date", F.col("id").desc())
)
