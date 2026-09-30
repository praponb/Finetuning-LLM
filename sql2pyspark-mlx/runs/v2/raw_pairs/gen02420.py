from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .filter(F.col("cost").between(270, 690))
    .select(
        "id",
        "brand",
        "supplier_id",
        "launch_date",
        "cost",
        F.sum("cost").over(w).alias("running_cost"),
    )
)
