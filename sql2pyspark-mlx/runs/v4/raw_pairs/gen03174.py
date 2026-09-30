from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("stock_qty").between(195, 383))
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .select(F.col("s.rating"), F.col("st.latest_launch_date"), F.col("st.max_cost"))
    .orderBy(F.col("max_cost").desc(), F.col("rating").desc())
)
